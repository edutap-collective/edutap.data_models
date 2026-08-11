import pytest

from edutap.data_models.runtime.errors import Unprocessable


def test_it_is_an_ordinary_exception_and_carries_its_reason():
    # The loop turns the exception into the `reason` of a dead letter entry with
    # str(), so whatever is raised here is what an operator reads later.
    error = Unprocessable("unknown action 'renew'")

    assert str(error) == "unknown action 'renew'"
    assert isinstance(error, Exception)


def test_nothing_else_is_unprocessable_by_accident():
    # The default direction is deliberate: an unclassified failure counts as the
    # world's fault, gets retried and is parked only if it persists. The reverse
    # default would discard messages for reasons nobody has looked at.
    with pytest.raises(RuntimeError):
        try:
            raise RuntimeError("database briefly away")
        except Unprocessable:  # pragma: no cover - must not catch
            pytest.fail("a plain error must not be treated as the message's fault")
