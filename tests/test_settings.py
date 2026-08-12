from pathlib import Path

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


def test_the_mtls_fields_are_named_after_the_variables_a_deployment_sets(monkeypatch):
    # The field names are the contract. Under a service's env_prefix they become
    # EDUTAP_KAFKA_CA_FILE and its siblings -- the same three the production overlay
    # already sets for the google callback handler. Renaming one here silently stops
    # a mounted secret from arriving, and the service falls back to plaintext against
    # a broker that only speaks SSL.
    class Prefixed(KafkaSettings):
        model_config = {"env_prefix": "EDUTAP_KAFKA_"}

    monkeypatch.setenv("EDUTAP_KAFKA_TOPIC_PREFIX", "edutap.test")
    monkeypatch.setenv("EDUTAP_KAFKA_CA_FILE", "/run/secrets/kafka_ca")
    monkeypatch.setenv("EDUTAP_KAFKA_CERT_FILE", "/run/secrets/kafka_client_cert")
    monkeypatch.setenv("EDUTAP_KAFKA_KEY_FILE", "/run/secrets/kafka_client_key")

    settings = Prefixed()

    assert settings.ca_file == Path("/run/secrets/kafka_ca")
    assert settings.cert_file == Path("/run/secrets/kafka_client_cert")
    assert settings.key_file == Path("/run/secrets/kafka_client_key")


def test_without_certificate_material_the_fields_stay_empty():
    # All three default together, and the default is the development case. A default
    # path would point at a file that is not there and turn every local run into a
    # deployment error.
    settings = KafkaSettings(topic_prefix="edutap.test")

    assert settings.ca_file is None
    assert settings.cert_file is None
    assert settings.key_file is None
    assert settings.password == ""


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
