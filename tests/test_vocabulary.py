from edutap.data_models import PassLifecycleState, WalletType


def test_values_are_their_names():
    # The database stores the value, not the name -- a mismatch between the two is
    # the failure mode this vocabulary exists to prevent.
    for member in list(WalletType) + list(PassLifecycleState):
        assert member.value == member.name


def test_wallet_type_distinguishes_product_not_only_vendor():
    # Apple VAS and Apple Access are different products with different provisioning;
    # collapsing them to "APPLE" is what made the older vocabularies unusable.
    assert WalletType.APPLE_VAS != WalletType.APPLE_ACCESS
