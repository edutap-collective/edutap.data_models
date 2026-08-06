from datetime import UTC, datetime, timedelta, timezone

import pytest

from edutap.data_models import messaging


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
