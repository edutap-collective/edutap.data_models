"""Arguments to `consume()` that would lose messages have to cost the start.

`attempts=0` was silent data loss: the retry range is empty, so no attempt is ever
made, no failure is ever raised, `_handle` returns "nothing to park" and the loop
commits a record no handler has seen. Nothing in the log, nothing in the dead letter
topic, and the message gone.
"""

import pytest

from edutap.data_models.runtime.consumer import consume

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeRecord:
    topic = "edutap.dev.pass.command"
    partition = 0
    offset = 1
    key = b"pass-1"
    value = b"{}"
    headers = ()


class FakeConsumer:
    def __init__(self) -> None:
        self.commits = 0
        self.stopped = False

    async def __aiter__(self):
        yield FakeRecord()

    async def commit(self) -> None:
        self.commits += 1

    async def stop(self) -> None:
        self.stopped = True


@pytest.fixture
def handled():
    return []


@pytest.fixture
def handler(handled):
    async def handle(record):
        handled.append(record)

    return handle


async def test_zero_attempts_is_refused_and_nothing_is_committed(handler, handled):
    consumer = FakeConsumer()

    with pytest.raises(ValueError, match="attempts must be at least 1"):
        await consume(consumer, handler, attempts=0)

    assert handled == []
    assert consumer.commits == 0


async def test_a_negative_attempt_count_is_refused_as_well(handler):
    consumer = FakeConsumer()

    with pytest.raises(ValueError, match="attempts must be at least 1"):
        await consume(consumer, handler, attempts=-1)

    assert consumer.commits == 0


async def test_a_negative_backoff_is_refused(handler):
    # `asyncio.sleep()` accepts a negative delay and returns immediately, so this
    # would not fail -- it would quietly mean "no backoff at all" while the caller
    # believes there is one.
    with pytest.raises(ValueError, match="backoff must not be negative"):
        await consume(FakeConsumer(), handler, backoff=-1)


async def test_a_single_attempt_is_still_allowed(handler, handled):
    # The boundary the guard must not overshoot: one attempt and no retry is a
    # legitimate configuration for a handler that is not worth retrying.
    consumer = FakeConsumer()

    await consume(consumer, handler, attempts=1)

    assert len(handled) == 1
    assert consumer.commits == 1
