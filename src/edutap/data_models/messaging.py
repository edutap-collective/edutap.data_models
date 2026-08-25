"""The Kafka message contract: header block, key rule and logical topic names.

Six packages produce or consume on the same bus. This module exists so the contract
is written once rather than six times -- six copies of a header block diverge, and
the divergence is invisible until a consumer silently stops recognising a message.

See the Kafka topic schema design record for why the names look as they do.
"""

from collections.abc import Iterable
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field

from .vocabulary import IssuanceState, WalletType

#: The producing service, e.g. ``apple_wallet_vas_web_service``.
#:
#: Set by the client and therefore **not** an authentication: it says what a service
#: claims. What can be relied on is who was allowed to write to the topic at all.
HEADER_PRODUCER = "edutap-producer"

#: Contract and version of the payload, e.g. ``pass-state/v1``.
HEADER_SCHEMA = "edutap-schema"

#: Idempotency key. A consumer that sees the same value twice has seen the same
#: event twice.
HEADER_EVENT_ID = "edutap-event-id"

#: When the event happened **at the sender**, RFC 3339.
#:
#: Load-bearing rather than decorative: the ``pass_state`` watermark compares against
#: it, so it has to stay stable across every retry. As a header it can be read
#: without deserialising the body.
HEADER_OCCURRED_AT = "edutap-occurred-at"

#: The discriminator where one topic carries several kinds of message:
#: ``create`` | ``update`` | ``deactivate`` on ``pass.command``,
#: ``registered`` | ``unregistered`` on ``device.registration``.
HEADER_ACTION = "edutap-action"

#: Image tag of the producing service. Optional.
HEADER_PRODUCER_VERSION = "edutap-producer-version"

#: Passed through across service boundaries. Optional.
HEADER_CORRELATION_ID = "edutap-correlation-id"

#: Logical topic names, without the environment prefix.
#:
#: The prefix is configuration (see :class:`edutap.data_models.settings.KafkaSettings`);
#: what a topic *is* belongs here. Three dimensions: ``person`` is the university's
#: internal path, ``pass`` the pass lifecycle, ``device`` the layer between a pass and
#: the devices holding it.
TOPIC_PERSON = "person"

#: What happened to a person after the fact, for consumers that hold data about one.
#:
#: Distinct from :data:`TOPIC_PERSON`, which is the *entrance*: that one carries a
#: trigger from an identity management system and is read by a spooler. This one is
#: written by that spooler once the view has changed, and it is read by services that
#: keep something of their own per person -- photographs, for instance -- and have no
#: other way to learn that a person is gone.
#:
#: Cut wider than "deleted" on purpose. The ``edutap-action`` header carries which
#: transition it was, so a later suspension or reactivation is a new action on this
#: topic rather than a thirteenth topic. The schema is deliberately small, and every
#: name in it is a contract with its consumers.
TOPIC_PERSON_LIFECYCLE = "person.lifecycle"

TOPIC_PASS_COMMAND = "pass.command"
TOPIC_PASS_STATE = "pass.state"
TOPIC_PASS_LOG = "pass.log"
TOPIC_DEVICE_NOTIFY = "device.notify"
TOPIC_DEVICE_REGISTRATION = "device.registration"

#: Payload contracts, by the name that goes into :data:`HEADER_SCHEMA`.
#:
#: Here for the same reason the header names are: the string is the contract, and a
#: contract written down in each of six packages is six chances to mistype it. It was
#: already on its way there -- ``lmu_edutap_worker`` carries all three as literals in
#: its own table of known schemas, and the producing side would have written them a
#: second time.
#:
#: Versioned, and the version is not decoration. A ``pass-command/v2`` is not a
#: ``pass-command/v1`` with extra fields to be ignored; it is a contract a given build
#: has never seen, and guessing at it is what the dead letter topic exists to prevent.
SCHEMA_PASS_COMMAND = "pass-command/v1"
SCHEMA_PASS_STATE = "pass-state/v1"
SCHEMA_DEVICE_REGISTRATION = "device-registration/v1"

#: The values :data:`HEADER_ACTION` may carry on :data:`TOPIC_PASS_COMMAND`.
#:
#: ``pass.state`` deliberately has none: the callback services normalise at the edge,
#: so that topic carries one kind of message and has nothing to discriminate.
ACTION_CREATE = "create"
ACTION_UPDATE = "update"
ACTION_DEACTIVATE = "deactivate"

#: Suffix of the dead letter queue belonging to an input topic.
DLQ_SUFFIX = "dlq"


def dlq_of(topic: str) -> str:
    """Return the dead letter topic belonging to ``topic``.

    One DLQ per *input topic*, not per source: several callback services feeding
    ``pass.state`` still share one ``pass.state.dlq``, and which of them wrote a
    given record is in its ``edutap-producer`` header.
    """
    return f"{topic}.{DLQ_SUFFIX}"


def build_headers(
    *,
    producer: str,
    schema: str,
    event_id: str,
    occurred_at: datetime,
    action: str | None = None,
    producer_version: str | None = None,
    correlation_id: str | None = None,
) -> list[tuple[str, bytes]]:
    """Build the header block every eduTAP message carries.

    Returns the shape ``aiokafka`` expects: a list of ``(name, bytes)`` pairs.

    ``occurred_at`` must be timezone-aware. A naive timestamp would be rendered
    without an offset, and the watermark comparison on the consumer side would then
    silently compare values from different clocks.
    """
    if occurred_at.tzinfo is None:
        raise ValueError("occurred_at must be timezone-aware")

    pairs: list[tuple[str, str]] = [
        (HEADER_PRODUCER, producer),
        (HEADER_SCHEMA, schema),
        (HEADER_EVENT_ID, event_id),
        (HEADER_OCCURRED_AT, occurred_at.astimezone(UTC).isoformat()),
    ]
    for name, value in (
        (HEADER_ACTION, action),
        (HEADER_PRODUCER_VERSION, producer_version),
        (HEADER_CORRELATION_ID, correlation_id),
    ):
        if value is not None:
            pairs.append((name, value))
    return [(name, value.encode()) for name, value in pairs]


def read_header(headers: Iterable[tuple[str, bytes]], name: str) -> str | None:
    """Return one header value as text, or ``None`` if it is absent."""
    for key, value in headers:
        if key == name:
            return value.decode()
    return None


class PassCommand(BaseModel):
    """The body of one ``pass.command`` message, schema :data:`SCHEMA_PASS_COMMAND`.

    An order from the issuing service to the pass-state consumer: this pass, for this
    person, in this wallet, is now in this issuance state. What kind of order it is
    rides in :data:`HEADER_ACTION` rather than here -- a consumer routing on the
    action can do so without deserialising a body, and the header is what the topic
    schema makes mandatory.

    That leaves ``action`` and :attr:`issuance_state` looking redundant, and they are
    not quite: the action says what happened, the state says what to store. They
    correspond today (``create`` → ``CREATED``, ``deactivate`` → ``REVOKED``) and the
    correspondence is the *issuer's* to decide, not this contract's to enforce.

    Field names follow ``edutap.db_definitions.public.tables.PassState`` so the
    consumer stores what it receives instead of translating it. What is deliberately
    **not** here:

    * ``holder_state`` and ``version`` -- the consumer owns both. The first is learned
      from ``pass.state``, the second is its watermark.
    * ``issuing_service`` -- it comes from the :data:`HEADER_PRODUCER` header. Which
      builder produced a pass is a fact about the sender, and a sender that could also
      *state* it in the body could state something else.
    """

    # `extra="forbid"` rather than ignoring unknown fields, and it is the same
    # decision the schema version encodes: a body carrying something this build has
    # never seen is not a v1 message with decoration, it is a message from a contract
    # this build does not know. Refused, parked, and visible -- rather than accepted
    # with the unknown half dropped on the floor.
    #
    # `frozen=True` because a received message is a fact. Nothing downstream has any
    # business editing what the sender said.
    model_config = ConfigDict(extra="forbid", frozen=True)

    #: The provider's pass identifier. Not a UUID field: Google Wallet object
    #: identifiers carry a prefix and a suffix, and HEIDI's UUID is HEIDI's to mint.
    #:
    #: `min_length=1` because the empty string is the shape an unset value takes when
    #: it travels through an f-string. It would be stored without complaint and match
    #: nothing ever again.
    pass_id: str = Field(min_length=1)

    #: Who the pass belongs to -- an identifier the university can resolve on its own,
    #: an ePPN or a UUID or a hash. Never interpreted here.
    #:
    #: Joined against ``person_view.person_uid`` without a foreign key, which is why
    #: an empty one has to be refused at the producer: downstream it would not raise,
    #: it would file a pass under a key that matches no person.
    person_uid: str = Field(min_length=1)

    wallet_type: WalletType

    issuance_state: IssuanceState

    #: Which template the pass was built from, and which variant of it. The variant is
    #: optional because most pass types have exactly one.
    pass_template: str = Field(min_length=1)
    pass_template_variant: str | None = None

    #: The issuing institution, as in eduPerson. Optional: a single-tenant deployment
    #: knows it without being told, and one that does not is the one that needs it.
    schac_home_organization: str | None = None
