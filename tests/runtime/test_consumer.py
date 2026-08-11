import pytest

from edutap.data_models.runtime.consumer import consume

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeRecord:
    def __init__(self, offset: int) -> None:
        self.topic = "edutap.dev.pass.state"
        self.partition = 0
        self.offset = offset
        self.key = b"pass-1"
        self.value = b"{}"
        self.headers = ()


class FakeConsumer:
    """Stands in for AIOKafkaConsumer: async-iterable, records its commits."""

    def __init__(self, records) -> None:
        self._records = list(records)
        self.commits = 0
        self.stopped = False

    async def __aiter__(self):
        for record in self._records:
            yield record

    async def commit(self) -> None:
        self.commits += 1

    async def stop(self) -> None:
        self.stopped = True


async def test_every_message_reaches_the_handler():
    seen = []
    consumer = FakeConsumer([FakeRecord(1), FakeRecord(2)])

    async def handler(record):
        seen.append(record)

    await consume(consumer, handler)

    assert [record.offset for record in seen] == [1, 2]


async def test_the_offset_is_committed_only_after_the_handler_returns():
    # Commits are manual on purpose. With auto-commit, a crash mid-handling advances
    # the offset anyway and the message is gone; here it is redelivered.
    order = []
    consumer = FakeConsumer([FakeRecord(1)])

    async def handler(record):
        order.append("handled")

    async def recording_commit():
        order.append("committed")

    consumer.commit = recording_commit

    await consume(consumer, handler)

    assert order == ["handled", "committed"]


async def test_a_failing_handler_does_not_commit_and_does_not_swallow():
    # Until the dead letter path exists, a failure has to be loud. Committing here
    # would lose the message silently, and catching it would hide that the scaffold
    # has no error path yet.
    consumer = FakeConsumer([FakeRecord(1)])

    async def handler(record):
        raise RuntimeError("no handler for this yet")

    with pytest.raises(RuntimeError):
        await consume(consumer, handler)

    assert consumer.commits == 0


async def test_the_consumer_is_stopped_even_when_the_handler_fails():
    consumer = FakeConsumer([FakeRecord(1)])

    async def handler(record):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await consume(consumer, handler)

    assert consumer.stopped is True


# The three tests around `consumer_options` stayed with the consuming service. They
# assert a group id and a broker address that come from that service's settings
# class, and the settings class is exactly what must not follow the loop into a
# library -- see `docs/superpowers/specs/2026-08-11-kafka-runtime-extraction.md`.
