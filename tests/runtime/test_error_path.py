"""The loop's behaviour when a message cannot be processed."""

import pytest

from edutap.data_models.runtime.consumer import consume
from edutap.data_models.runtime.errors import Unprocessable

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeRecord:
    def __init__(self, offset: int = 1) -> None:
        self.topic = "edutap.dev.pass.command"
        self.partition = 0
        self.offset = offset
        self.key = b"pass-1"
        self.value = b"{}"
        self.headers = ()


class FakeConsumer:
    def __init__(self, records) -> None:
        self._records = list(records)
        self.commits = 0
        self.stopped = False
        self.log: list[str] = []

    async def __aiter__(self):
        for record in self._records:
            yield record

    async def commit(self) -> None:
        self.commits += 1
        self.log.append("commit")

    async def stop(self) -> None:
        self.stopped = True


class FakeDlq:
    def __init__(self, consumer=None, fails: bool = False) -> None:
        self.entries = []
        self._fails = fails
        self._consumer = consumer

    async def publish(self, record, *, reason: str) -> None:
        if self._fails:
            raise RuntimeError("dead letter write failed")
        self.entries.append(reason)
        if self._consumer is not None:
            self._consumer.log.append("dlq")


async def test_an_unprocessable_message_goes_to_the_dead_letter_and_is_committed():
    # Committed *after* the dead letter write, and committed at all: leaving the
    # offset behind would spin the loop on the same message for ever, which is the
    # partition block the guard rails warn about.
    consumer = FakeConsumer([FakeRecord()])
    dlq = FakeDlq(consumer)

    async def handler(record):
        raise Unprocessable("unknown action 'renew'")

    await consume(consumer, handler, dlq=dlq)

    assert dlq.entries == ["unknown action 'renew'"]
    assert consumer.commits == 1


async def test_the_dead_letter_write_happens_before_the_commit():
    # The rule everything else hangs on. A crash between the two must lose nothing:
    # committed-then-written would drop the message if the write never happened.
    consumer = FakeConsumer([FakeRecord()])
    dlq = FakeDlq(consumer)

    async def handler(record):
        raise Unprocessable("nope")

    await consume(consumer, handler, dlq=dlq)

    assert consumer.log == ["dlq", "commit"]


async def test_a_failing_dead_letter_write_stops_the_loop_without_committing():
    # Neither handled nor parked. The only honest answer is to stop and let the
    # message be redelivered after a restart.
    consumer = FakeConsumer([FakeRecord()])

    async def handler(record):
        raise Unprocessable("nope")

    with pytest.raises(RuntimeError):
        await consume(consumer, handler, dlq=FakeDlq(fails=True))

    assert consumer.commits == 0


async def test_a_transient_failure_is_retried_and_then_parked():
    # A database that is briefly away is not a broken message. It is retried, and
    # only a failure that survives every attempt is parked -- otherwise a five second
    # outage would fill the dead letter topic with perfectly good messages.
    attempts = []
    consumer = FakeConsumer([FakeRecord()])
    dlq = FakeDlq(consumer)

    async def handler(record):
        attempts.append(1)
        raise RuntimeError("database gone")

    await consume(consumer, handler, dlq=dlq, attempts=3, backoff=0)

    assert len(attempts) == 3
    assert len(dlq.entries) == 1
    assert consumer.commits == 1


async def test_an_unprocessable_message_is_not_retried():
    # Retrying a malformed message three times is three times the same answer. The
    # distinction between the two error kinds is what makes the retry budget useful.
    attempts = []
    consumer = FakeConsumer([FakeRecord()])

    async def handler(record):
        attempts.append(1)
        raise Unprocessable("not json")

    await consume(consumer, handler, dlq=FakeDlq(consumer), attempts=3, backoff=0)

    assert len(attempts) == 1


async def test_a_transient_failure_that_passes_on_a_retry_is_not_parked():
    attempts = []
    consumer = FakeConsumer([FakeRecord()])
    dlq = FakeDlq(consumer)

    async def handler(record):
        attempts.append(1)
        if len(attempts) < 2:
            raise RuntimeError("database gone")

    await consume(consumer, handler, dlq=dlq, attempts=3, backoff=0)

    assert len(attempts) == 2
    assert dlq.entries == []
    assert consumer.commits == 1


async def test_without_a_dead_letter_queue_nothing_is_swallowed():
    # The scaffold's behaviour, kept reachable: no dlq means a failure ends the loop
    # rather than being parked somewhere that does not exist.
    consumer = FakeConsumer([FakeRecord()])

    async def handler(record):
        raise Unprocessable("nope")

    with pytest.raises(Unprocessable):
        await consume(consumer, handler)

    assert consumer.commits == 0
