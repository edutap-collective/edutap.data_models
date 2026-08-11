"""What the loop over one Kafka consumer does, and what it needs that consumer to be.

Only the mechanics live here. How a consumer is configured and built belongs to the
consuming service: that is where the settings class, the broker address and the
knowledge of which schema a topic carries are, and none of those may follow the loop
into a library.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable
from typing import Any, Protocol

import structlog

from .errors import Unprocessable

log = structlog.get_logger(__name__)


class Consumer(Protocol):
    """What :func:`consume` needs a consumer to be.

    Narrower than ``AIOKafkaConsumer`` on purpose: an async iterable that can commit
    and can stop. Writing it down is what lets a test pass a twenty-line fake instead
    of a broker, and it states exactly which three pieces of that large class this
    loop actually depends on.

    It is also what keeps ``aiokafka`` out of this package. The driver is a
    dependency of the service that builds the consumer, not of the loop that reads
    from it.
    """

    def __aiter__(self) -> AsyncIterator[Any]:
        """Yield records as they arrive."""
        ...

    async def commit(self) -> None:
        """Commit the offsets of everything handed out so far."""
        ...

    async def stop(self) -> None:
        """Leave the group and release the connection."""
        ...


class Handler(Protocol):
    """What a topic's handler has to be: something awaitable, taking one record.

    A protocol rather than a base class, so a handler can be a plain function, a
    method, or an object with state, and a test can pass a two-line coroutine.
    """

    def __call__(self, record: Any, /) -> Awaitable[None]:
        """Take one Kafka record and do whatever this topic requires with it.

        Positional-only: this loop never names the argument, and requiring a name
        would exclude every handler that spells it differently.
        """
        ...


class DeadLetter(Protocol):
    """Somewhere to park a message the loop cannot process."""

    async def publish(self, record: Any, *, reason: str) -> None:
        """Write one entry and wait for the broker to confirm it."""
        ...


async def consume(
    consumer: Consumer,
    handler: Handler,
    *,
    dlq: DeadLetter | None = None,
    attempts: int = 3,
    backoff: float = 0.5,
) -> None:
    """Hand every record to ``handler``, park what fails, commit what is done with.

    The order is the whole point, and it is not interchangeable: **the dead letter
    entry is written and confirmed before the offset is committed.** A crash between
    the two then loses nothing -- the message is redelivered. The reverse would
    commit a message that never reached the dead letter topic, and nobody would ever
    learn that it existed.

    Two error kinds, opposite treatment. An :class:`~edutap.data_models.runtime.
    errors.Unprocessable` is the message's own fault and is parked on the first
    attempt: retrying a malformed payload is three times the same answer. Everything
    else is treated as the world's fault, retried with an exponential backoff, and
    parked only if it survives every attempt -- otherwise a five second database
    outage would fill the dead letter topic with perfectly good records.

    Without a ``dlq`` nothing is swallowed: a failure ends the loop, which is the
    right behaviour while there is nowhere to park anything.

    The consumer is stopped on the way out either way, so a broker does not have to
    wait for the session timeout to notice this member is gone.
    """
    try:
        async for record in consumer:
            reason = await _handle(record, handler, attempts=attempts, backoff=backoff, dlq=dlq)
            # Both conditions hold together by construction -- _handle only returns a
            # reason when there is a dead letter queue, and raises otherwise. Naming
            # both anyway costs nothing and saves a ty-ignore that nobody would
            # re-check later.
            if reason is not None and dlq is not None:
                await dlq.publish(record, reason=reason)
            await consumer.commit()
    finally:
        await consumer.stop()


async def _handle(
    record: Any,
    handler: Handler,
    *,
    attempts: int,
    backoff: float,
    dlq: "DeadLetter | None",
) -> str | None:
    """Run the handler; return why the message has to be parked, or ``None``.

    Raises rather than returning when there is no dead letter queue -- see
    :func:`consume`.
    """
    for attempt in range(1, attempts + 1):
        try:
            await handler(record)
            return None
        except Unprocessable as error:
            if dlq is None:
                raise
            return str(error)
        except Exception as error:  # noqa: BLE001 - classified below, not swallowed
            if dlq is None:
                raise
            if attempt == attempts:
                return f"{type(error).__name__}: {error}"
            log.warning(
                "handler failed, retrying",
                topic=record.topic,
                offset=record.offset,
                attempt=attempt,
                of=attempts,
                error=str(error),
            )
            if backoff:
                await asyncio.sleep(backoff * 2 ** (attempt - 1))
    return None  # pragma: no cover - the loop always returns or raises
