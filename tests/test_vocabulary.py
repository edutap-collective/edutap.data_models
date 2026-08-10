import warnings

import pytest

from edutap.data_models import (
    HolderState,
    InstanceState,
    IssuanceState,
    Provider,
    WalletType,
)


def test_values_are_their_names():
    # The database stores the value, not the name -- a mismatch between the two is
    # the failure mode this vocabulary exists to prevent.
    members = list(WalletType) + list(IssuanceState) + list(HolderState) + list(InstanceState)
    for member in members:
        assert member.value == member.name


def test_wallet_type_distinguishes_product_not_only_vendor():
    # Apple VAS and Apple Access are different products with different provisioning;
    # collapsing them to "APPLE" is what made the older vocabularies unusable.
    assert WalletType.APPLE_VAS != WalletType.APPLE_ACCESS


def test_the_three_axes_overlap_only_where_they_are_meant_to():
    # What the issuer wants, what the holder has, and what one exemplar is doing are
    # three questions, and a value on two axes is worth naming rather than
    # forbidding. Both overlaps here are deliberate and mean different things:
    # SUSPENDED, because "instances exist but none is usable" is exactly the summary
    # of an instance state; FAILED, because issuing a pass and provisioning one
    # exemplar of it can fail independently -- a pass that never got out at all is
    # not the same as one that reached three devices and lost the fourth.
    issuance = {member.value for member in IssuanceState}
    holder = {member.value for member in HolderState}
    instance = {member.value for member in InstanceState}
    assert issuance & holder == set()
    assert issuance & instance == {"FAILED"}
    assert holder & instance == {"SUSPENDED"}


def test_issuance_state_carries_no_provisioning_step():
    # INSTALL_PENDING and DELETE_PENDING are states of a provisioning run, not of an
    # issuer's intent. Their presence is what made the superseded vocabulary
    # Apple-Access-shaped and unusable for a .pkpass or a Google object.
    values = {member.value for member in IssuanceState}
    assert "INSTALL_PENDING" not in values
    assert "DELETE_PENDING" not in values
    assert "UPDATE_PENDING" not in values


def test_holder_state_is_a_summary_of_three_outcomes():
    # Derived, never set by a caller: at least one active instance, none active but
    # one suspended, or nothing at all.
    assert {member.value for member in HolderState} == {"NOT_PRESENT", "PRESENT", "SUSPENDED"}


def test_instance_state_separates_who_removed_the_pass():
    # The holder deleting a pass and the issuer withdrawing it look identical in a
    # single-axis vocabulary, and the difference is what a report is asked for.
    assert InstanceState.REMOVED_BY_HOLDER != InstanceState.REMOVED_BY_ISSUER


def test_provider_survives_the_move():
    assert Provider.GOOGLE.value == "GOOGLE"


def test_superseded_vocabulary_warns_on_use():
    # Kept reachable so a package still importing it fails loudly rather than
    # silently getting a different set of values, and dropped from the package's
    # exports so nothing new can pick it up by accident.
    from edutap import data_models

    assert not hasattr(data_models, "PassLifecycleState")

    with pytest.warns(DeprecationWarning, match="IssuanceState"):
        from edutap.data_models.vocabulary import PassLifecycleState

    assert PassLifecycleState.ACTIVE.value == "ACTIVE"


def test_unknown_attribute_still_raises_attribute_error():
    # The deprecation hook must not turn every typo into a warning and a None.
    from edutap.data_models import vocabulary

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(AttributeError):
            vocabulary.PassLifecyleState  # noqa: B018  deliberate typo
