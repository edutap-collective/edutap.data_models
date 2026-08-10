import pytest
from pydantic import ValidationError

from edutap.data_models.settings import KafkaSettings, ServiceSettings


def test_topic_prefix_has_no_default():
    # Without a value the service must not start. Falling back to a default would
    # mean writing into another environment's topics without anyone noticing.
    with pytest.raises(ValidationError):
        KafkaSettings()


def test_topic_composes_prefix_and_logical_name():
    settings = KafkaSettings(topic_prefix="edutap.production")
    assert settings.topic("pass.state") == "edutap.production.pass.state"


def test_environment_defaults_to_production():
    # An unset environment must not masquerade as development: the value labels
    # every error report and every exported span, and "unknown" filed under
    # development is the one direction in which nobody goes looking.
    assert ServiceSettings().environment == "production"


def test_telemetry_is_on_unless_switched_off():
    # Instrumentation that is off by default is instrumentation nobody notices is
    # broken. Where the data goes is decided by the endpoint, not by this flag; the
    # flag exists so a test run or a flooding service can be silenced explicitly.
    assert ServiceSettings().telemetry_enabled is True
    assert ServiceSettings(telemetry_enabled=False).telemetry_enabled is False


def test_log_level_rejects_a_typo():
    # A misspelled level would otherwise be accepted and then silently fall back to
    # whatever the logging library defaults to.
    assert ServiceSettings().log_level == "INFO"
    with pytest.raises(ValidationError):
        ServiceSettings(log_level="VERBOSE")


def test_sentry_settings_are_gone_from_this_package():
    # They moved to edutap.observability_settings together with the wiring they
    # belong to; two copies of one settings class is the drift this package exists
    # to prevent.
    from edutap.data_models import settings

    assert not hasattr(settings, "SentrySettings")
