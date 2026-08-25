"""The producing half of the runtime: how a message is configured and sent.

The consuming half has been here since 0.2.0. What was missing is the side that
*writes*, and its absence is why `lmu_edutap_backend` sends a body with no envelope
at all -- the header block was available, calling it was optional, and the optional
call is the one that gets forgotten.
"""

from datetime import UTC, datetime

import pytest

from edutap.data_models import messaging
from edutap.data_models.runtime.producer import producer_options, publish

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeProducer:
    def __init__(self, fails: bool = False) -> None:
        self.sent = []
        self._fails = fails

    async def send_and_wait(self, topic, *, key=None, value=None, headers=None):
        if self._fails:
            raise RuntimeError("broker unavailable")
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": headers})


@pytest.fixture
def producer():
    return FakeProducer()


def _publish(producer, **kw):
    base = dict(
        topic="edutap.dev.pass.command",
        key=b"pass-1",
        value=b'{"pass_id": "pass-1"}',
        producer_name="lmu_edutap_backend",
        schema=messaging.SCHEMA_PASS_COMMAND,
        event_id="evt_1",
        occurred_at=datetime(2026, 8, 25, 10, 0, tzinfo=UTC),
        action=messaging.ACTION_CREATE,
    )
    return publish(producer, **(base | kw))


# --- producer_options -------------------------------------------------------


def test_options_are_returned_not_applied():
    # Same shape the consuming side uses: a test can assert the exact set without a
    # broker, and the caller decides what to merge into it.
    assert producer_options("kafka:9092") == {
        "bootstrap_servers": "kafka:9092",
        "acks": "all",
        "enable_idempotence": True,
    }


def test_acks_all_is_not_negotiable():
    """The HTTP caller gets its answer on the strength of this write.

    Acknowledged by the leader alone means a leader that dies before its replicas
    catch up takes the record with it -- after the endpoint has already told a person
    their pass exists.
    """
    assert producer_options("kafka:9092")["acks"] == "all"


def test_the_dead_letter_producer_is_configured_the_same_way():
    """One definition, two callers. The DLQ needs exactly these durability options,
    and a second copy of them is a second thing to keep in step."""
    from edutap.data_models.runtime.dlq import dead_letter_options

    assert dead_letter_options("kafka:9092") == producer_options("kafka:9092")


# --- publish ----------------------------------------------------------------


async def test_the_envelope_is_built_here_not_by_the_caller(producer):
    """The reason this function exists at all.

    `build_headers()` has been available since 0.2.0 and calling it was optional.
    A caller that forgets produces a body the broker accepts and every consumer
    refuses -- success at the producer, dead letter at the consumer. Folding the two
    calls into one removes the failure mode rather than documenting it.
    """
    await _publish(producer)

    names = [name for name, _ in producer.sent[0]["headers"]]
    assert names[:4] == [
        messaging.HEADER_PRODUCER,
        messaging.HEADER_SCHEMA,
        messaging.HEADER_EVENT_ID,
        messaging.HEADER_OCCURRED_AT,
    ]
    assert messaging.HEADER_ACTION in names


async def test_key_value_and_topic_are_passed_through(producer):
    await _publish(producer)

    assert producer.sent[0]["topic"] == "edutap.dev.pass.command"
    assert producer.sent[0]["key"] == b"pass-1"
    assert producer.sent[0]["value"] == b'{"pass_id": "pass-1"}'


async def test_a_naive_timestamp_is_refused_before_the_broker_is_touched(producer):
    """The watermark on `pass_state` compares against this value across services.

    Refused here rather than at the consumer: a message that has already been
    accepted by the broker cannot be unsent, and the consumer's only remaining move
    is the dead letter topic.
    """
    with pytest.raises(ValueError, match="timezone-aware"):
        await _publish(producer, occurred_at=datetime(2026, 8, 25, 10, 0))

    assert producer.sent == []


async def test_optional_headers_are_omitted_not_emptied(producer):
    await _publish(producer, action=None)

    names = [name for name, _ in producer.sent[0]["headers"]]
    assert messaging.HEADER_ACTION not in names


async def test_correlation_and_version_reach_the_record(producer):
    await _publish(producer, producer_version="2026-08-25_1200", correlation_id="corr-9")

    headers = producer.sent[0]["headers"]
    assert messaging.read_header(headers, messaging.HEADER_PRODUCER_VERSION) == "2026-08-25_1200"
    assert messaging.read_header(headers, messaging.HEADER_CORRELATION_ID) == "corr-9"


async def test_a_failed_send_is_not_swallowed():
    """The caller decides what a lost write means; this layer must not decide for it.

    In `lmu_edutap_backend` it means the endpoint fails after the pass already
    exists -- deliberately, because a silently dropped command would leave a pass
    that the database never hears about.
    """
    producer = FakeProducer(fails=True)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        await _publish(producer)


async def test_it_waits_for_the_broker_rather_than_queueing(producer):
    """`send_and_wait`, not `send`. An endpoint that returns before the broker has
    confirmed has told a person something it does not know."""
    await _publish(producer)

    assert len(producer.sent) == 1


# --- the consumer's rules, restated -----------------------------------------
#
# `lmu_edutap_worker.headers.read_envelope()` is the gate every message passes. Its
# rules are restated here as data rather than imported, and that is not a shortcut:
# this package must not know a service, and a test that imported one would make the
# dependency rule false in the one direction it exists to prevent.
#
# Restating them means they can drift. That is the trade, and the drift is visible --
# the worker's own suite checks the same rules against the same strings, both sides
# read from `messaging`, and a change to the contract fails one of the two.

MANDATORY = (
    messaging.HEADER_PRODUCER,
    messaging.HEADER_SCHEMA,
    messaging.HEADER_EVENT_ID,
    messaging.HEADER_OCCURRED_AT,
)
KNOWN_SCHEMAS = {messaging.TOPIC_PASS_COMMAND: {messaging.SCHEMA_PASS_COMMAND}}
ALLOWED_ACTIONS = {
    messaging.TOPIC_PASS_COMMAND: {
        messaging.ACTION_CREATE,
        messaging.ACTION_UPDATE,
        messaging.ACTION_DEACTIVATE,
    }
}


def _read_envelope(headers, *, logical_topic):
    """What the worker does, restated. Raises like it does."""
    for name in MANDATORY:
        value = messaging.read_header(headers, name)
        if value is None or not value.strip():
            raise AssertionError(f"mandatory header {name} is missing")

    schema = messaging.read_header(headers, messaging.HEADER_SCHEMA)
    if schema not in KNOWN_SCHEMAS.get(logical_topic, set()):
        raise AssertionError(f"unknown schema {schema!r} on {logical_topic}")

    raw_time = messaging.read_header(headers, messaging.HEADER_OCCURRED_AT)
    occurred_at = datetime.fromisoformat(raw_time)
    if occurred_at.tzinfo is None:
        raise AssertionError("occurred_at has no time zone")

    allowed = ALLOWED_ACTIONS.get(logical_topic, set())
    action = messaging.read_header(headers, messaging.HEADER_ACTION)
    if allowed and action not in allowed:
        raise AssertionError(f"unknown action {action!r} on {logical_topic}")
    return occurred_at


@pytest.mark.parametrize(
    "action",
    [messaging.ACTION_CREATE, messaging.ACTION_UPDATE, messaging.ACTION_DEACTIVATE],
)
async def test_a_published_message_passes_the_consumers_gate(producer, action):
    """The acceptance criterion of the whole exercise.

    What `lmu_edutap_backend` sends today fails at the first mandatory header and is
    parked, while the producer reports success. A message from `publish()` gets
    through.
    """
    await _publish(producer, action=action)

    occurred_at = _read_envelope(
        producer.sent[0]["headers"], logical_topic=messaging.TOPIC_PASS_COMMAND
    )

    assert occurred_at == datetime(2026, 8, 25, 10, 0, tzinfo=UTC)


async def test_the_gate_really_does_reject_a_bare_body(producer):
    """The counter-test, so the one above cannot pass by being toothless.

    This is what the estate's one existing producer sends: value and key, no headers.
    """
    await producer.send_and_wait("edutap.dev.pass.command", key=b"pass-1", value=b"{}")

    with pytest.raises(AssertionError, match="mandatory header"):
        _read_envelope(
            producer.sent[0]["headers"] or (), logical_topic=messaging.TOPIC_PASS_COMMAND
        )


async def test_an_action_outside_the_set_would_be_parked(producer):
    """`publish()` does not police the action -- which topic allows which value is the
    consumer's table, and a library that enforced it would have to know every topic.
    The gate is where it is caught, and this proves the gate is the one that catches.
    """
    await _publish(producer, action="delete")

    with pytest.raises(AssertionError, match="unknown action"):
        _read_envelope(producer.sent[0]["headers"], logical_topic=messaging.TOPIC_PASS_COMMAND)
