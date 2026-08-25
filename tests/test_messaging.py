from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from edutap.data_models import messaging
from edutap.data_models.vocabulary import IssuanceState, WalletType


def _headers(**kw):
    base = dict(
        producer="apple_wallet_vas_web_service",
        schema="pass-state/v1",
        event_id="evt_1",
        occurred_at=datetime(2026, 8, 7, 10, 0, tzinfo=UTC),
    )
    return messaging.build_headers(**(base | kw))


def test_mandatory_block_is_always_present():
    names = [name for name, _ in _headers()]
    assert names == [
        messaging.HEADER_PRODUCER,
        messaging.HEADER_SCHEMA,
        messaging.HEADER_EVENT_ID,
        messaging.HEADER_OCCURRED_AT,
    ]


def test_optional_headers_are_omitted_not_emptied():
    # An empty value is a value; a consumer cannot tell it from "not set".
    assert messaging.read_header(_headers(), messaging.HEADER_ACTION) is None
    assert messaging.read_header(_headers(action="create"), messaging.HEADER_ACTION) == "create"


def test_occurred_at_must_be_aware():
    with pytest.raises(ValueError, match="timezone-aware"):
        _headers(occurred_at=datetime(2026, 8, 7, 10, 0))


def test_occurred_at_is_normalised_to_utc():
    # Two producers in different zones must yield comparable timestamps -- the
    # watermark on the consumer side compares them directly.
    other = timezone(timedelta(hours=2))
    value = messaging.read_header(
        _headers(occurred_at=datetime(2026, 8, 7, 12, 0, tzinfo=other)),
        messaging.HEADER_OCCURRED_AT,
    )
    assert value == "2026-08-07T10:00:00+00:00"


def test_dlq_belongs_to_the_input_topic():
    assert messaging.dlq_of(messaging.TOPIC_PASS_STATE) == "pass.state.dlq"


# --- the pass.command payload ----------------------------------------------


def _command(**kw):
    base = dict(
        pass_id="3f2a1c00-0000-4000-8000-000000000001",
        person_uid="a1b2c3d4@lmu.de",
        wallet_type=WalletType.APPLE_VAS,
        issuance_state=IssuanceState.CREATED,
        pass_template="student_id_v1",
    )
    return messaging.PassCommand(**(base | kw))


def test_the_body_carries_what_pass_state_stores():
    command = _command()

    assert command.pass_id == "3f2a1c00-0000-4000-8000-000000000001"
    assert command.wallet_type is WalletType.APPLE_VAS
    assert command.issuance_state is IssuanceState.CREATED


def test_the_variant_and_the_organisation_are_optional():
    """Most pass types have one variant, and a single-tenant deployment knows its own
    organisation without being told."""
    command = _command()

    assert command.pass_template_variant is None
    assert command.schac_home_organization is None


def test_an_unknown_field_is_refused_rather_than_ignored():
    """The same decision the schema version encodes.

    A body carrying something this build has never seen is not a v1 message with
    decoration -- it is a message from a contract this build does not know. Accepting
    it with the unknown half dropped is how two services quietly stop agreeing.
    """
    with pytest.raises(ValidationError):
        _command(holder_state="PRESENT")


def test_an_empty_identifier_is_refused_at_the_producer():
    """The shape an unset value takes when it travels through an f-string.

    Downstream it would not raise: `pass_state.person_uid` has no foreign key onto
    `person_view`, so an empty one files a pass under a key that matches no person
    and nothing reports it.
    """
    with pytest.raises(ValidationError):
        _command(person_uid="")
    with pytest.raises(ValidationError):
        _command(pass_id="")


def test_a_wallet_type_outside_the_vocabulary_is_refused():
    # Vocabulary values are API -- a typo here would be stored and then compared
    # against for the life of the deployment.
    with pytest.raises(ValidationError):
        _command(wallet_type="APPLE")


def test_the_message_cannot_be_edited_after_the_fact():
    """A received message is a fact. Nothing downstream may rewrite what the sender
    said and pass it on as if it had."""
    command = _command()

    with pytest.raises(ValidationError):
        command.issuance_state = IssuanceState.REVOKED


def test_it_round_trips_through_json():
    # This is how it reaches the broker and comes back.
    command = _command(pass_template_variant="mensa", schac_home_organization="lmu.de")

    assert messaging.PassCommand.model_validate_json(command.model_dump_json()) == command


def test_the_deactivation_carries_revoked_not_a_holder_state():
    """The issuer decides about the issuance, not about the object in someone's
    wallet. `InstanceState.REMOVED_BY_ISSUER` would be a claim about a device."""
    command = _command(issuance_state=IssuanceState.REVOKED)

    assert command.issuance_state is IssuanceState.REVOKED


# --- the contract strings ---------------------------------------------------


def test_the_schema_names_carry_their_version():
    # A consumer matches on the exact string; an unversioned one would make a
    # breaking change indistinguishable from a compatible one.
    assert messaging.SCHEMA_PASS_COMMAND == "pass-command/v1"
    assert messaging.SCHEMA_PASS_STATE == "pass-state/v1"
    assert messaging.SCHEMA_DEVICE_REGISTRATION == "device-registration/v1"


def test_the_actions_of_pass_command_are_the_ones_consumers_know():
    """`lmu_edutap_worker` accepts exactly these three on this topic. They live here
    so the producing side does not write them out a second time."""
    assert {
        messaging.ACTION_CREATE,
        messaging.ACTION_UPDATE,
        messaging.ACTION_DEACTIVATE,
    } == {"create", "update", "deactivate"}
