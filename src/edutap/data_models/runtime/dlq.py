"""Where a message goes when it cannot be processed.

One dead letter topic per **input topic**, named `<topic>.dlq`. Not per producer:
several callback services feed `pass.state` and still share one `pass.state.dlq` --
which of them wrote a given record is in its `edutap-producer` header.

The entry has one job: make the message replayable. So the entry **is** the message.
The value, the key and the original headers are passed through byte for byte, and
the diagnosis rides alongside in headers of its own. Replaying an entry is then a
copy -- read it, produce it back onto its original topic -- with nothing to unwrap
and nothing to decode.

Only the mechanics live here; they are written against a producer protocol, so the
Kafka driver stays a dependency of the consuming service.
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

import structlog

from ..messaging import dlq_of

log = structlog.get_logger(__name__)

#: Diagnostic headers added alongside the original ones.
#:
#: A separate prefix on purpose: the original block is passed through untouched, so
#: `edutap-producer` still names who *sent* the message rather than who parked it,
#: and `edutap-event-id` still identifies the message rather than the entry. A
#: replay therefore needs to strip nothing -- these headers are additive, and a
#: consumer that ignores them sees exactly the message that failed.
HEADER_DLQ_REASON = "edutap-dlq-reason"
HEADER_DLQ_TOPIC = "edutap-dlq-origin-topic"
HEADER_DLQ_PARTITION = "edutap-dlq-origin-partition"
HEADER_DLQ_OFFSET = "edutap-dlq-origin-offset"
HEADER_DLQ_PARKED_AT = "edutap-dlq-parked-at"
HEADER_DLQ_PARKED_BY = "edutap-dlq-parked-by"


class Producer(Protocol):
    """What :class:`DeadLetterQueue` needs: something that publishes and confirms."""

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


def dead_letter_options(bootstrap_servers: str) -> dict[str, Any]:
    """Return how the dead letter producer is configured, before it is built.

    The broker address is passed in rather than read from a settings object: which
    settings class a service has is its own business, and a library that knows one
    of them knows a service.

    ``acks="all"`` is the load-bearing entry: the offset is committed on the strength
    of this write. Acknowledged by the leader alone means a leader that dies before
    its replicas catch up takes the entry with it -- after the loop has already moved
    past the message.
    """
    return {
        "bootstrap_servers": bootstrap_servers,
        "acks": "all",
        "enable_idempotence": True,
    }


class DeadLetterQueue:
    """Parks messages the loop cannot process."""

    def __init__(self, producer: Producer, *, parked_by: str) -> None:
        """Wrap a producer. One per process, like the consumers.

        ``parked_by`` names the service doing the parking. It is passed in rather
        than fixed here because which service parked an entry is that service's
        answer to give, and the entry has to be able to say it without claiming to
        have *sent* the message.
        """
        self._producer = producer
        self._parked_by = parked_by

    async def publish(self, record: Any, *, reason: str) -> None:
        """Park one message, byte for byte, with the reason beside it.

        The entry **is** the message: same value, same key, same headers. Replaying
        it means producing it back onto its original topic -- nothing to unwrap and
        nothing to decode, which is what makes a dead letter worth having.

        Failures are not caught. The caller commits the offset only after this
        returns, and swallowing a failure here would commit a message that reached
        neither the handler nor this topic.
        """
        topic = dlq_of(record.topic)
        await self._producer.send_and_wait(
            topic,
            key=record.key,
            value=record.value,  # verbatim -- None stays None, bytes stay bytes
            headers=[
                *(record.headers or ()),
                (HEADER_DLQ_REASON, reason.encode("utf-8")),
                (HEADER_DLQ_TOPIC, record.topic.encode("utf-8")),
                (HEADER_DLQ_PARTITION, str(record.partition).encode("ascii")),
                (HEADER_DLQ_OFFSET, str(record.offset).encode("ascii")),
                (HEADER_DLQ_PARKED_AT, datetime.now(tz=UTC).isoformat().encode("ascii")),
                (HEADER_DLQ_PARKED_BY, self._parked_by.encode("utf-8")),
            ],
        )
        log.warning(
            "message parked",
            dlq=topic,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            reason=reason,
        )
