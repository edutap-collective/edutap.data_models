"""Running several consumers side by side, and ending together.

The whole design is one decision: **the process is the unit of failure.** Several
consumers run in one container; if any of them stops, for any reason, the others are
cancelled and the process exits non-zero. The orchestrator restarts it.

The alternative -- catching the error and rebuilding that one consumer -- was
rejected. A service with two of three consumers alive looks healthy to the
orchestrator and quietly stops processing one topic, and there is no signal anywhere
that says so. A container in a restart loop is visible; a half-dead one is not.

That places one requirement on the deployment, and it is not optional: the service
must be allowed to restart indefinitely. A ``restart_policy`` with ``max_attempts``
turns a broker that is briefly unreachable into a service that stays down.
"""

import asyncio
import contextlib
import signal
from collections.abc import Awaitable, Callable

import structlog

log = structlog.get_logger(__name__)


async def run_until_one_stops(runners: list[Callable[[], Awaitable[None]]]) -> None:
    """Run every callable concurrently; the first one to end ends them all.

    ``asyncio.TaskGroup`` gives exactly that semantics: an exception in one child
    cancels the siblings and is re-raised, wrapped in an ``ExceptionGroup``. A child
    that *returns* is the second case and needs help -- a ``TaskGroup`` is happy to
    let it finish and wait for the rest, but for a consumer that is a stopped topic
    with a live process, so it is turned into an error too.

    An empty list is the third case and the quietest one. A ``TaskGroup`` with no
    children returns immediately, so a service that wired up no consumer at all would
    shut down cleanly with exit code 0 -- indistinguishable, to the orchestrator, from
    a completed rolling update. A service whose whole job is to consume and which
    consumes nothing has failed, and it has to say so.
    """
    if not runners:
        raise ValueError("no consumers to run; a service that consumes nothing is misconfigured")

    async def guard(runner: Callable[[], Awaitable[None]]) -> None:
        await runner()
        raise RuntimeError(f"{getattr(runner, '__name__', runner)} returned; a consumer must not")

    async with asyncio.TaskGroup() as group:
        for runner in runners:
            group.create_task(guard(runner))


def install_signal_handlers(stop: asyncio.Event) -> None:
    """Turn SIGTERM and SIGINT into a set event rather than an abrupt teardown.

    Swarm sends SIGTERM and waits ten seconds before SIGKILL. Ten seconds is plenty
    to leave the consumer group cleanly, which spares the remaining members a
    rebalance that only ends when the session times out.
    """
    loop = asyncio.get_running_loop()
    for number in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(number, stop.set)


async def serve(runners: list[Callable[[], Awaitable[None]]]) -> None:
    """Run the consumers until one stops or the orchestrator asks us to.

    A stop signal is not a failure: it returns normally, and the exit code stays
    zero, so a rolling update does not read as a crash.
    """
    stop = asyncio.Event()
    install_signal_handlers(stop)

    consumers = asyncio.create_task(run_until_one_stops(runners))
    stopping = asyncio.create_task(stop.wait())

    done, pending = await asyncio.wait([consumers, stopping], return_when=asyncio.FIRST_COMPLETED)

    if stopping in done and consumers not in done:
        log.info("stop requested, shutting down")
        consumers.cancel()
        try:
            await consumers
        except asyncio.CancelledError:
            # We asked for it; the task answering with CancelledError is the answer.
            pass
        except BaseExceptionGroup as group:
            # A cancelled TaskGroup re-raises what its children raised on the way out,
            # wrapped. The cancellations are ours and expected -- anything else is a
            # consumer that failed *while* shutting down, and swallowing that with the
            # rest is how a broken shutdown path stays invisible for a year.
            _, unexpected = group.split(asyncio.CancelledError)
            if unexpected is not None:
                raise unexpected from None
        return

    for task in pending:
        task.cancel()
        # Awaited, not merely cancelled: cancel() only *requests* the cancellation.
        # Dropping the task before the loop has delivered it leaves it pending at
        # garbage collection, and asyncio then logs "Task was destroyed but it is
        # pending!" -- noise arriving at exactly the moment somebody is reading the
        # log to find out why the process ended.
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await consumers  # re-raises, which is the point
