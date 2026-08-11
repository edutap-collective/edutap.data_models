import asyncio

import pytest

from edutap.data_models.runtime.runner import run_until_one_stops

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_the_consumers_run_side_by_side():
    # All three are started before any of them has finished anything -- concurrently,
    # not one after the other. They are then cancelled, because a consumer that
    # returns is an error in this design and cannot be used to end the test.
    started = []
    all_running = asyncio.Event()

    async def consumer(name):
        started.append(name)
        if len(started) == 3:
            all_running.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        run_until_one_stops(
            [
                lambda: consumer("pass.command"),
                lambda: consumer("pass.state"),
                lambda: consumer("device.registration"),
            ]
        )
    )
    await asyncio.wait_for(all_running.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert sorted(started) == ["device.registration", "pass.command", "pass.state"]


async def test_one_failure_brings_the_whole_process_down():
    # Fail fast, deliberately. A worker with two of three consumers alive looks
    # healthy to Swarm and quietly stops processing one topic; the restart is the
    # repair, and a container in a restart loop is visible where a half-dead one is
    # not.
    cancelled = []

    async def forever(name):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(name)
            raise

    async def failing():
        raise RuntimeError("rebalance failed")

    with pytest.raises(ExceptionGroup) as caught:
        await run_until_one_stops([lambda: forever("pass.state"), failing])

    assert cancelled == ["pass.state"]
    # A TaskGroup raises an ExceptionGroup, whose own str() names only the group. The
    # point is that the original error survives inside it rather than being swallowed,
    # so it has to be looked for there -- an assertion against str(group) would pass
    # for an empty group just as happily.
    assert [str(error) for error in caught.value.exceptions] == ["rebalance failed"]


async def test_a_consumer_returning_early_also_ends_the_run():
    # An `async for` over a stopped consumer ends without raising. That is not a
    # normal state for a service meant to run forever, so it must not leave the
    # other two running and the process alive.
    ended = []

    async def ends_immediately():
        ended.append("ended")

    async def forever():
        await asyncio.Event().wait()

    with pytest.raises(ExceptionGroup) as caught:
        await run_until_one_stops([ends_immediately, forever])

    assert "must not" in str(caught.value.exceptions[0])

    assert ended == ["ended"]
