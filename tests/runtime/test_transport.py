import ssl

import pytest

from edutap.data_models.runtime.transport import transport_options
from edutap.data_models.settings import KafkaSettings


def settings(**overrides) -> KafkaSettings:
    """A settings object with the one required field filled in."""
    return KafkaSettings(topic_prefix="edutap.test", **overrides)


def test_without_certificate_material_the_transport_stays_plain():
    # The development case, and the only one in which plaintext is a decision rather
    # than an accident: nothing is configured, so nothing is claimed.
    assert transport_options(settings()) == {}


def test_all_three_files_produce_an_ssl_transport(client_material):
    # security_protocol has to be named explicitly. aiokafka defaults to PLAINTEXT
    # and would ignore an ssl_context handed to it on its own.
    options = transport_options(settings(**client_material))

    assert options["security_protocol"] == "SSL"
    assert isinstance(options["ssl_context"], ssl.SSLContext)


def test_the_broker_certificate_is_verified(client_material):
    # A context that skips verification would connect to anything answering on the
    # port. mTLS proves the client to the broker; this is the other direction, and
    # it is the half a misconfigured context silently drops.
    context = transport_options(settings(**client_material))["ssl_context"]

    assert context.verify_mode is ssl.CERT_REQUIRED
    assert context.check_hostname is True


@pytest.mark.parametrize("missing", ["ca_file", "cert_file", "key_file"])
def test_a_partial_configuration_aborts_instead_of_falling_back(client_material, missing):
    # The case this function exists for. Against an SSL-only broker a silent fallback
    # to plaintext does not produce "no encryption", it produces a connection that
    # fails during the handshake -- and the message names the broker rather than the
    # secret nobody mounted. A missing secret is a deployment error and has to read
    # like one.
    #
    # Parametrised rather than written once: an `all()` that forgets one of the three
    # still passes a test that only ever removes the same one.
    del client_material[missing]

    with pytest.raises(ValueError) as excinfo:
        transport_options(settings(**client_material))

    assert missing in str(excinfo.value)


def test_a_file_that_is_not_on_disk_is_reported_as_such(client_material, tmp_path):
    # All three are configured, so the deployment believes it mounted them. Letting
    # ssl raise here surfaces as a bare FileNotFoundError from inside the driver,
    # naming a path but not what it was for.
    client_material["cert_file"] = tmp_path / "absent.pem"

    with pytest.raises(ValueError) as excinfo:
        transport_options(settings(**client_material))

    assert "absent.pem" in str(excinfo.value)
    assert "cert_file" in str(excinfo.value)


def test_the_key_password_reaches_the_context(encrypted_client_material, key_password):
    # What this pins is that the field is not dropped on the way. The wrong password
    # failing is ssl doing its job; the right one having no effect would be ours.
    with pytest.raises(ValueError):
        transport_options(settings(**encrypted_client_material, password="wrong"))

    options = transport_options(settings(**encrypted_client_material, password=key_password))
    assert options["security_protocol"] == "SSL"


def test_an_unencrypted_key_needs_no_password(client_material):
    # The default is the empty string, and an empty password is not the same as no
    # password: handed to load_cert_chain it makes OpenSSL reject a key that has no
    # encryption at all.
    assert settings().password == ""

    options = transport_options(settings(**client_material))
    assert options["security_protocol"] == "SSL"
