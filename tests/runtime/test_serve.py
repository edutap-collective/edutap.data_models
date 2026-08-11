"""The shutdown path: what a stop signal does, and what a misconfiguration does.

Everything here was uncovered until a review found three ways for the runtime to end
quietly. The module docstring of `runner` calls this behaviour contract -- the process
is the unit of failure, a stop signal is not a failure -- so it is tested like one.
"""

import asyncio
import signal

import pytest

from edutap.data_models.runtime.runner import install_signal_handlers, run_until_one_stops, serve

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def signal_handlers_removed():
    """Hand the signals back to the interpreter once the test is done.

    `serve()` installs handlers and never removes them -- correct for a process that
    is meant to die with them, wrong to leave behind in a test run, where the next
    SIGTERM would be delivered to a callback belonging to a loop that has closed.

    Async on purpose: the teardown needs the loop that installed the handlers to still
    be running, and a synchronous fixture is torn down after it is gone.
    """
    yield
    loop = asyncio.get_running_loop()
    for number in (signal.SIGTERM, signal.SIGINT):
        loop.remove_signal_handler(number)


async def _forever() -> None:
    await asyncio.Event().wait()


async def test_no_consumers_at_all_is_a_failure_not_a_clean_exit():
    # An empty TaskGroup finishes immediately, so this used to shut down with exit
    # code 0: to the orchestrator, indistinguishable from a completed rolling update.
    # A service whose whole job is to consume and which consumes nothing has failed.
    with pytest.raises(ValueError, match="no consumers"):
        await run_until_one_stops([])


async def test_serve_refuses_an_empty_runner_list_too():
    # The same misconfiguration through the front door. It must not be possible to
    # reach the quiet exit by calling the outer function instead of the inner one.
    with pytest.raises(ValueError, match="no consumers"):
        await serve([])


async def test_a_stop_signal_ends_the_service_without_an_error(signal_handlers_removed):
    # A rolling update must not read as a crash: SIGTERM returns normally, and the
    # exit code stays zero. If this raised, every deployment would look like an
    # incident in whatever watches the orchestrator.
    served = asyncio.create_task(serve([_forever, _forever]))
    await _wait_until_handlers_are_installed()

    signal.raise_signal(signal.SIGTERM)

    await asyncio.wait_for(served, timeout=2)

    assert served.exception() is None


async def test_a_stop_signal_leaves_no_task_behind(signal_handlers_removed):
    # This path already cancelled *and* awaited what it left behind, so this test
    # passes before the fix as well. It is here as the guard for the half that was
    # right, next to the half that was not -- see the failing-consumer test below.
    before = asyncio.all_tasks()

    served = asyncio.create_task(serve([_forever]))
    await _wait_until_handlers_are_installed()
    signal.raise_signal(signal.SIGTERM)
    await asyncio.wait_for(served, timeout=2)

    assert _leftover(before) == set()


async def test_a_failing_consumer_leaves_no_task_behind(signal_handlers_removed):
    # This is the path that leaked. A consumer fails, the process is meant to die, and
    # the task waiting for the stop signal is still pending -- it was cancelled but
    # never awaited, and `cancel()` only *requests* a cancellation. The task was then
    # collected while pending and asyncio logged "Task was destroyed but it is
    # pending!" on top of the crash that is actually worth reading.
    #
    # That message goes through the asyncio logger, not the warnings module, so
    # `-W error` does not catch it. Asserting on the tasks is the deterministic form
    # of the same question.
    before = asyncio.all_tasks()

    async def failing():
        raise RuntimeError("rebalance failed")

    with pytest.raises(ExceptionGroup):
        await serve([failing, _forever])

    assert _leftover(before) == set()


async def test_signal_handlers_turn_a_signal_into_a_set_event(signal_handlers_removed):
    # Swarm sends SIGTERM and waits ten seconds before SIGKILL. Ten seconds is plenty
    # to leave the consumer group cleanly, which is only possible if the signal sets
    # an event instead of tearing the process down where it stands.
    stop = asyncio.Event()
    install_signal_handlers(stop)

    signal.raise_signal(signal.SIGINT)

    await asyncio.wait_for(stop.wait(), timeout=2)

    assert stop.is_set()


async def _wait_until_handlers_are_installed() -> None:
    """Yield until `serve()` has replaced the default disposition of SIGTERM.

    Waiting for the evidence rather than for a fixed number of scheduling steps: an
    unhandled SIGTERM terminates the test run, so this must not race.
    """
    for _ in range(1000):
        if signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
            return
        await asyncio.sleep(0)
    raise AssertionError("serve() did not install its signal handlers")


def _leftover(before: set[asyncio.Task]) -> set[asyncio.Task]:
    """Return the tasks that are still pending and were not there beforehand."""
    return asyncio.all_tasks() - before - {asyncio.current_task()}
