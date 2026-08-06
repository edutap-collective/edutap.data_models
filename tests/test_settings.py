import pytest
from pydantic import ValidationError

from edutap.data_models.settings import KafkaSettings, SentrySettings


def test_topic_prefix_has_no_default():
    # Without a value the service must not start. Falling back to a default would
    # mean writing into another environment's topics without anyone noticing.
    with pytest.raises(ValidationError):
        KafkaSettings()


def test_topic_composes_prefix_and_logical_name():
    settings = KafkaSettings(topic_prefix="edutap.production")
    assert settings.topic("pass.state") == "edutap.production.pass.state"


def test_sentry_is_off_unless_configured():
    assert SentrySettings().sentry_dsn == ""
