import pytest

from edutap.data_models.runtime.dlq import DeadLetterQueue, dead_letter_options

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeRecord:
    def __init__(self, *, value=b'{"broken": true}', key=b"pass-1", headers=(), offset=1) -> None:
        self.topic = "edutap.dev.pass.command"
        self.partition = 2
        self.offset = offset
        self.key = key
        self.value = value
        self.headers = headers


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


async def test_it_writes_to_the_dead_letter_of_the_input_topic(producer):
    # One DLQ per input topic, named <topic>.dlq. Not per producer: several services
    # feed pass.state and still share one pass.state.dlq -- who wrote a record is in
    # its header.
    await DeadLetterQueue(producer, parked_by="svc").publish(FakeRecord(), reason="unknown action")

    assert producer.sent[0]["topic"] == "edutap.dev.pass.command.dlq"


async def test_the_entry_is_the_message_byte_for_byte(producer):
    """A replay is a copy, not an unpacking. That is the whole point."""
    record = FakeRecord(value=b'{"dn": "cn=A,ou=Benutzer,o=uni-muenchen,c=de"}')

    await DeadLetterQueue(producer, parked_by="svc").publish(record, reason="bad schema")

    assert producer.sent[0]["value"] == record.value
    assert producer.sent[0]["key"] == record.key


async def test_a_body_that_is_not_utf8_survives_untouched(producer):
    """The broken encoding is why this message is here. Passing bytes through means
    there is nothing that could fail on it."""
    record = FakeRecord(value=b"\xff\xfe not text")

    await DeadLetterQueue(producer, parked_by="svc").publish(record, reason="x")

    assert producer.sent[0]["value"] == b"\xff\xfe not text"


async def test_a_tombstone_stays_a_tombstone(producer):
    """The envelope could not express the difference between None and empty."""
    await DeadLetterQueue(producer, parked_by="svc").publish(FakeRecord(value=None), reason="x")

    assert producer.sent[0]["value"] is None


async def test_the_original_headers_are_passed_through(producer):
    """`edutap-producer` still names who sent the message, not who parked it -- a
    replayed message has to keep its identity."""
    record = FakeRecord(headers=[("edutap-producer", b"lmu_edutap_vzd_webhook")])

    await DeadLetterQueue(producer, parked_by="svc").publish(record, reason="x")

    assert ("edutap-producer", b"lmu_edutap_vzd_webhook") in producer.sent[0]["headers"]


async def test_the_diagnosis_rides_alongside_in_its_own_namespace(producer):
    record = FakeRecord()

    await DeadLetterQueue(producer, parked_by="svc").publish(record, reason="bad schema")

    headers = dict(producer.sent[0]["headers"])
    assert headers["edutap-dlq-reason"] == b"bad schema"
    assert headers["edutap-dlq-origin-offset"] == b"1"
    assert headers["edutap-dlq-parked-by"] == b"svc"


async def test_a_failing_dead_letter_write_raises():
    # It must not be swallowed. The caller commits only after this returns, and a
    # swallowed failure here would commit a message that reached neither the handler
    # nor the dead letter topic.
    with pytest.raises(RuntimeError):
        await DeadLetterQueue(FakeProducer(fails=True), parked_by="svc").publish(
            FakeRecord(), reason="x"
        )


def test_the_dead_letter_producer_waits_for_every_replica():
    # Same reason as the VZD webhook's producer: the offset is committed on the
    # strength of this write. Acknowledged by the leader alone is not enough.
    options = dead_letter_options("kafka:9092")

    assert options["acks"] == "all"
    assert options["enable_idempotence"] is True
